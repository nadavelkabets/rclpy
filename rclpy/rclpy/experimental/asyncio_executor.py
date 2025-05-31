import asyncio
import time
from contextlib import ExitStack, contextmanager
from functools import partial
from typing import (Any, Callable, Coroutine, Generator, Optional, Set, Type,
                    TypeVar, Union)

from rclpy.client import Client
from rclpy.constants import S_TO_NS
from rclpy.executors import BaseExecutor, ExternalShutdownException, TracebackType, await_or_execute
from rclpy.events import set_executor
from rclpy.logging import get_logger
from rclpy.node import Node
from rclpy.service import Service
from rclpy.subscription import Subscription
from rclpy.timer import Timer

import rclpy
from rclpy.utilities import get_default_context
from rclpy.context import Context
import traceback

EntityT = TypeVar("EntityT", bound=Union[Subscription, Service, Client, Timer])


def _is_timer_destroyed(timer: Timer):
    return timer.handle.pointer == 0


class AsyncioExecutor(BaseExecutor[asyncio.Future, asyncio.Task]):
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

        set_executor(self)

    @property
    def context(self) -> Context:
        """Get the context associated with the executor."""
        return self._context

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

    def shutdown(self, close_loop: bool = True) -> None:
        """
        Clear all nodes and close the event loop.
        """
        self._nodes.clear()
        self._update_entities_from_nodes()

        for task in self._tasks:
            task.cancel()

        if self._loop.is_running():
            self._loop.stop()
        elif not self._loop.is_closed() and close_loop:
            self._loop.close()

        set_executor(None)

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    @contextmanager
    def _timeout(self, timeout: int) -> Generator[None, None, None]:
        handle = self._loop.call_later(timeout, self._loop.stop)
        yield

        if handle.when() > time.time():
            handle.cancel()

    @contextmanager
    def _stop_after_callback(self) -> Generator[None, None, None]:
        self._should_stop_after_callback = True

        yield

        self._should_stop_after_callback = False
        if self._stop_handle:
            self._stop_handle.cancel()
            self._stop_handle = None

    def _on_future_complete(self, _: asyncio.Future):
        self._loop.stop()
    
    def spin(
        self,
        once: bool = False,
        future: Optional[asyncio.Future] = None,
        timeout: Optional[float] = None
    ) -> None:
        if not self._context.ok():
            return
        
        with ExitStack() as context:
            if once:
                context.enter_context(self._stop_after_callback())
            if timeout:
                context.enter_context(self._timeout(timeout))
            if future is not None:
                future.add_done_callback(self._on_future_complete)

            self._loop.run_forever()

        if not self._context.ok():
            raise ExternalShutdownException()

    def spin_once(self, timeout: Optional[float] = None) -> None:
        self.spin(once=True, timeout=timeout)

    def spin_once_until_future_complete(
        self, future: asyncio.Future, timeout: Optional[float] = None
    ) -> None: 
        self.spin(once=True, future=future, timeout=timeout)

    # TODO: should this function accept an asyncio Future or an rclpy Future?
    def spin_until_future_complete(
        self, future: asyncio.Future, timeout: Optional[float] = None
    ) -> None:
        self.spin(future=future, timeout=timeout)

    def create_task(
        self, callback: Union[Callable, Coroutine], *args: Any, **kwargs: Any
    ) -> asyncio.Task:
        if not asyncio.iscoroutine(callback):
            callback = await_or_execute(callback, *args, **kwargs)

        return self._loop.create_task(callback)

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
        asyncio_future = self._loop.create_future()
        
        return asyncio_future

    # TODO: optimize this function to run less times?
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

        if self._update_entity_set(
            self._timers,
            timers,
            lambda s: s.set_on_reset_callback(self._handle_timer_reset),
            lambda s: s.clear_on_reset_callback(),
        ):
            self._update_timers()

    def _handle_timer_reset(self, _: int):
        self._loop.call_soon_threadsafe(self._update_timers)

    def _update_entity_set(
        self,
        current_set: set[EntityT],
        new_set: set[EntityT],
        added_cb: Callable[[EntityT], None],
        removed_cb: Callable[[EntityT], None],
    ) -> bool:
        added_entities = new_set - current_set
        for e in added_entities:
            current_set.add(e)
            e.handle.__enter__()
            added_cb(e)

        removed_entities = current_set - new_set
        for e in removed_entities:
            removed_cb(e)
            e.handle.__exit__(None, None, None)
            current_set.remove(e)

        return added_entities or removed_entities

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

    def _update_timers(self):
        if self._update_timers_handle and not self._update_timers_handle.cancelled():
            self._update_timers_handle.cancel()
        
        timers = list(self._timers)
        next_jump_time_seconds = None
        for timer in timers:
            if _is_timer_destroyed(timer) or timer.is_canceled():
                continue

            if timer.is_ready():
                with timer.handle:
                    timer.handle.call_timer()
                
                self._loop.call_soon(timer.callback)
            
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
