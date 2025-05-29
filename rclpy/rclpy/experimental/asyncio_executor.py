import asyncio
import time
from contextlib import ExitStack, contextmanager
from functools import partial
from typing import (Any, Callable, Coroutine, Generator, Optional, Set, Type,
                    TypeVar, Union)

from rclpy.client import Client
from rclpy.constants import S_TO_NS
from rclpy.duration import Duration
from rclpy.executors import ExecutorBase, ExternalShutdownException, TracebackType, await_or_execute
from rclpy.logging import get_logger
from rclpy.node import Node
from rclpy.service import Service
from rclpy.subscription import Subscription
from rclpy.timer import Timer

import rclpy
from rclpy.utilities import get_default_context
from rclpy.context import Context
import traceback

EntityT = TypeVar("EntityT", bound=Union[Subscription, Service, Client])


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
    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        *,
        context: Optional[Context] = None
    ) -> None: 
        self._loop = loop or self._get_loop()
        self._context = context or get_default_context()
        self._context.on_shutdown(self.shutdown)

        self._tasks: Set[asyncio.Task] = set()
        self._nodes: Set[Node] = set()
        self._subscriptions: Set[Subscription] = set()
        self._clients: Set[Client] = set()
        self._services: Set[Service] = set()
        self._timers: Set[Timer] = set()
        
        self._should_stop_after_callback = False
        self._stop_handle: Optional[asyncio.Handle] = None
        self._update_timers_handle: Optional[asyncio.Handle] = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

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

        if self._loop.is_running():
            self._loop.stop()

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    @contextmanager
    def _timeout(self, timeout: int) -> Generator[None, None, None]:
        handle = None
        if timeout:
            handle = self._loop.call_later(timeout, self._loop.stop)
        yield

        if handle and handle.when() > time.time():
            handle.cancel()

    def _spin(
        self,
        once: bool = False,
        future: Optional[asyncio.Future] = None,
        timeout: Optional[int] = None
    ):
        if not self._context.ok():
            return
        
        with ExitStack() as context:
            if once:
                context.enter_context(self._stop_after_callback())
            if timeout:
                context.enter_context(self._timeout(timeout))

            if future:
                self._loop.run_until_complete(future)
            else:
                self._loop.run_forever()

        if not self._context.ok():
            raise ExternalShutdownException()

    def spin(self) -> None:
        self._spin()

    @property
    def context(self) -> Context:
        """Get the context associated with the executor."""
        return self._context

    def spin_once(self, timeout: Optional[int] = None) -> None:
        self._spin(once=True, timeout=timeout)

    @contextmanager
    def _stop_after_callback(self) -> Generator[None, None, None]:
        self._should_stop_after_callback = True

        yield

        self._should_stop_after_callback = False
        if self._stop_handle:
            self._stop_handle.cancel()
            self._stop_handle = None

    def spin_once_until_future_complete(
        self, future: asyncio.Future, timeout: Optional[int] = None
    ) -> None: 
        self._spin(once=True, future=future, timeout=timeout)

    # TODO: should this function accept an asyncio Future or a rclpy Future?
    def spin_until_future_complete(
        self, future: asyncio.Future, timeout: Optional[int] = None
    ) -> None:
        self._spin(future=future, timeout=timeout)

    def create_task(
        self, callback: Union[Callable, Coroutine], *args: Any, **kwargs: Any
    ) -> asyncio.Task:
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

    # TODO: should optimize this function to run less times?
    def _update_entities_from_nodes(self) -> None:
        subscriptions, clients, services, timers = set(), set(), set(), set()
        for node in self._nodes:
            subscriptions.update(node.subscriptions)
            clients.update(node.clients)
            services.update(node.services)
            timers.update(node.timers)

        self._update_entity_set(
            self._subscriptions,
            subscriptions,
            lambda s: s.set_on_new_message_callback(partial(self._handle_ready_subscription, s)),
            lambda s: s.clear_on_new_message_callback(),
        )

        self._update_entity_set(
            self._clients,
            clients,
            lambda c: c.set_on_new_response_callback(partial(self._handle_ready_client, c)),
            lambda c: c.clear_on_new_response_callback(),
        )

        self._update_entity_set(
            self._services,
            services,
            lambda s: s.set_on_new_request_callback(partial(self._handle_ready_service, s)),
            lambda s: s.clear_on_new_request_callback(),
        )

        self._update_entity_set(
            self._timers,
            timers,
            lambda s: s.set_on_reset_callback(lambda n: self._update_timers()),
            lambda s: s.set_on_reset_callback(),
        )

        if self._timers:
            self._update_timers()

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

    def _handle_ready_subscription(self, subscription: Subscription, number_of_events: int) -> None:
        self._loop.call_soon_threadsafe(
            self._handle_ready_entity, self._take_subscription, subscription, number_of_events
        )

    def _handle_ready_client(self, client: Client, number_of_events: int) -> None:
        self._loop.call_soon_threadsafe(
            self._handle_ready_entity, self._take_client, client, number_of_events
        )

    def _handle_ready_service(self, service: Service, number_of_events: int) -> None:
        self._loop.call_soon_threadsafe(
            self._handle_ready_entity, self._take_service, service, number_of_events
        )

    def _execute_ready_timer(self, timer: Timer):
        with timer.handle:
            timer.handle.call_timer()

        async def wrapped_callback():
            # try:
            await timer.callback()
            # except Exception:
            #     get_logger(timer.get_logger_name()).error(traceback.format_exc())

        task = self._loop.create_task(wrapped_callback())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.remove)

    def _update_timers(self):
        if self._update_timers_handle and not self._update_timers_handle.cancelled():
            self._update_timers_handle.cancel()
        
        next_jump_time_seconds = None
        for timer in self._timers:
            if timer.is_ready():
                self._execute_ready_timer(timer)
            
            timer_next_jump_time = timer.time_until_next_call() / S_TO_NS
            if next_jump_time_seconds is None:
                next_jump_time_seconds = timer_next_jump_time
            else:
                next_jump_time_seconds = min(
                    next_jump_time_seconds,
                    timer_next_jump_time
                )

        if next_jump_time_seconds and not self._loop.is_closed():
            self._update_timers_handle = self._loop.call_later(next_jump_time_seconds, self._update_timers)

    def _handle_ready_entity(
        self,
        take_entity_callback: Callable[[EntityT], Optional[Coroutine]],
        entity: EntityT,
        number_of_events: int,
    ) -> None:
        if not self._context.ok():
            raise ExternalShutdownException()
        
        for _ in range(number_of_events):
            callback = take_entity_callback(entity)
            if not callback:
                break
            
            async def wrapped_callback():
                try:
                    await callback()
                except Exception:
                    get_logger(entity.get_logger_name()).error(traceback.format_exc())

            task = self._loop.create_task(wrapped_callback())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.remove)

        if self._should_stop_after_callback and not self._stop_handle:
            self._stop_handle = self._loop.call_soon(self._loop.stop)
