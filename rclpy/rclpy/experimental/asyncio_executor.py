import asyncio
from functools import partial
from typing import Any, Callable, Coroutine, Optional, Set, TypeVar, Union, Type, Generator
from contextlib import contextmanager
import time

import rclpy
from rclpy.node import Node
from rclpy.subscription import Subscription
from rclpy.client import Client
from rclpy.service import Service
from rclpy.executors import ExecutorBase, TracebackType, await_or_execute

EntityT = TypeVar("EntityT", bound=Union[Subscription, Service, Client])

@contextmanager
def _timeout(
    timeout: int,
    loop: asyncio.AbstractEventLoop
) -> Generator[None, None, None]:
    handle = None
    if timeout:
        handle = loop.call_later(timeout, loop.stop)
    yield

    if handle and handle.when() > time.time():
        handle.cancel()

def _chain_future(rclpy_future: rclpy.Future, asyncio_future: asyncio.Future) -> None:
    """Chain two futures so that when one completes, so does the other.

    The result (or exception) of source will be copied to destination.
    If destination is cancelled, source gets cancelled too.
    """

    def _call_check_cancel(asyncio_future: asyncio.Future):
        if asyncio_future.cancelled():
            rclpy_future.cancel()

    def _call_set_state(rclpy_future: rclpy.Future):
        if asyncio_future.cancelled():
            return
        if rclpy_future.cancelled():
            asyncio_future.cancel()
        else:
            exception = rclpy_future.exception()
            if exception is not None:
                asyncio_future.set_exception(exception)
            else:
                result = rclpy_future.result()
                asyncio_future.set_result(result)

    asyncio_future.add_done_callback(_call_check_cancel)
    rclpy_future.add_done_callback(_call_set_state)

class AsyncioExecutor(ExecutorBase):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self._tasks: Set[asyncio.Task] = set()
        self._nodes: Set[Node] = set()
        self._subscriptions: Set[Subscription] = set()
        self._clients: Set[Client] = set()
        self._services: Set[Service] = set()
        self._loop = loop or self._get_loop()

        self._should_stop_after_callback = False
        self._stop_handle: Optional[asyncio.Handle] = None

    def __enter__(self) -> 'AsyncioExecutor':
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.shutdown()

    def shutdown(self) -> None:
        """
        Clear all nodes and close the event loop.
        """
        self._nodes.clear()
        self._update_entities_from_nodes()
        
        for task in self._tasks:
            task.cancel()

        if not self._loop.is_running():
            self._loop.close()

        self._tasks.clear()
        self._loop = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:     
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    def _execute_entity(self, coro: Coroutine) -> None:
        task = self._loop.create_task(coro)
        task.add_done_callback(self._exception_handler)
        self._tasks.add(task)


    def _exception_handler(self, fut) -> None:
        ex = fut.exception()
        if ex:
            raise ex

        self._tasks.remove(fut)

    def spin(self) -> None:
        self._loop.run_forever()

    def spin_once(self, timeout) -> None:
        with _timeout(timeout, self._loop):
            self._loop.run_forever()

    @contextmanager
    def _stop_after_callback(self) -> Generator[None, None, None]:
        self._should_stop_after_callback = True
        
        yield

        self._should_stop_after_callback = False
        self._stop_handle.cancel()
        self._stop_handle = None

    def spin_once_until_future_complete(self, future: asyncio.Future, timeout: Optional[int] = None) -> None:
        with self._stop_after_callback():
            with _timeout(timeout, self._loop):
                self._loop.run_until_complete(future)

    def spin_until_future_complete(self, future: asyncio.Future, timeout: Optional[int] = None) -> None:
        with _timeout(timeout, self._loop):
            self._loop.run_until_complete(future)

    def create_task(self, callback: Union[Callable, Coroutine], *args: Any, **kwargs: Any) -> asyncio.Task:
        if not asyncio.iscoroutine(callback):
            callback = await_or_execute(callback, *args, **kwargs)

        return self._loop.create_task(callback)

    def call_soon(self, callback: Callable, *args: Any, **kwargs: Any) -> asyncio.Handle:
        return self._loop.call_soon(callback, *args, **kwargs)

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

    def create_future(self) -> asyncio.Future:
        return self._loop.create_future()
    
    def wrap_future(self, rclpy_future: rclpy.Future) -> asyncio.Future:
        asyncio_future = self._loop.create_future()
        _chain_future(rclpy_future, asyncio_future)
        return asyncio_future

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

        if self._should_stop_after_callback and not self._stop_handle:
            self._stop_handle = self._loop.call_soon(self._loop.stop)
