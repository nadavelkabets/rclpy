import asyncio
from functools import partial
from typing import Any, Callable, Coroutine, Optional, Set, TypeVar, Union

from rclpy.node import Node
from rclpy.subscription import Subscription
from rclpy.client import Client
from rclpy.service import Service
from rclpy.executors import ExecutorBase

EntityT = TypeVar("EntityT", bound=Union[Subscription, Service, Client])


class AsyncioExecutor(ExecutorBase):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self._tasks: Set[asyncio.Task] = set()
        self._nodes: Set[Node] = set()
        self._subscriptions: Set[Subscription] = set()
        self._clients: Set[Client] = set()
        self._services: Set[Service] = set()
        self._loop = loop

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
        subscriptions, clients, services = set(), set(), set()
        for node in self._nodes:
            subscriptions.update(node.subscriptions)
            clients.update(node.clients)
            services.update(node.services)

        self._update_entity_set(
            self._subscriptions,
            subscriptions,
            lambda s: s.set_on_new_message_callback(
                    partial(self._handle_ready_subscription, s)
            ),
            lambda s: s.clear_on_new_message_callback(),
        )

        self._update_entity_set(
            self._clients,
            clients,
            lambda c: c.set_on_new_response_callback(
                    partial(self._handle_ready_client, c)
            ),
            lambda c: c.clear_on_new_response_callback(),
        )

        self._update_entity_set(
            self._services,
            services,
            lambda s: s.set_on_new_request_callback(
                    partial(self._handle_ready_service, s)
            ),
            lambda s: s.clear_on_new_request_callback(),
        )

    def _handle_ready_subscription(
        self,
        subscription: Subscription,
        number_of_events: int
    ) -> None:
        self._loop.call_soon_threadsafe(
            callback=self._handle_ready_entity,
            take_entity_callback=self._take_subscription,
            entity=subscription,
            number_of_events=number_of_events
        )
        
    def _handle_ready_client(
        self,
        client: Client,
        number_of_events: int
    ) -> None:
        self._loop.call_soon_threadsafe(
            callback=self._handle_ready_entity,
            take_entity_callback=self._take_client,
            entity=client,
            number_of_events=number_of_events
        )

    def _handle_ready_service(
        self,
        service: Service,
        number_of_events: int
    ) -> None:
        self._loop.call_soon_threadsafe(
            callback=self._handle_ready_entity,
            take_entity_callback=self._take_service,
            entity=service,
            number_of_events=number_of_events
        )

    def _handle_ready_entity(
        self,
        take_entity_callback: Callable[[EntityT], Optional[Coroutine]],
        entity: EntityT,
        number_of_events: int
    ) -> None:
        for _ in range(number_of_events):
            coro = take_entity_callback(entity)
            if not coro:
                break

            self._execute_entity(coro())

    def _update_entity_set(
        self,
        current_set: set,
        new_set: set,
        added_cb: Callable[[EntityT], None],
        removed_cb: Callable[[EntityT], None],
    ) -> None:
        # Handle additions
        for h in new_set - current_set:
            current_set.add(h)
            added_cb(h)

        # Handle removals
        for h in current_set - new_set:
            current_set.remove(h)
            removed_cb(h)
