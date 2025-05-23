from types import TracebackType
from typing import Any, Callable, Optional, Type, Set
from rclpy.executors import ExecutorBase
import asyncio
from rclpy.impl.implementation_singleton import rclpy_implementation as _rclpy
import time
from contextlib import contextmanager
import rclpy
from rclpy.executors import await_or_execute
from rclpy.node import Node
from rclpy.subscription import Subscription
from rclpy.timer import Timer
from rclpy.client import Client
from rclpy.service import Service
from functools import partial

@contextmanager
def _timeout(timeout: int, callback: Callable[[], None], loop: asyncio.AbstractEventLoop):
    handle = None
    if timeout:
        handle = loop.call_later(timeout, callback)
    yield

    if handle and handle.when() > time.time():
        handle.cancel()

class AsyncioExecutor(ExecutorBase):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.__executor = _rclpy.AsyncioExecutor()
        self._tasks: Set[asyncio.Task] = set()
        self._nodes: Set[Node] = set()
        self._subscriptions: Set[Subscription] = set()
        self._timers: Set[Timer] = set()
        self._clients: Set[Client] = set()
        self._services: Set[Service] = set()
        self._stop_after_user_callback = False
        if loop:
            self._loop = loop
        else:
            self._set_loop()
        self._attach_to_loop()

    def get_loop(self):
        return self._loop

    def __enter__(self):
        return self

    def __del__(self):
        del self.__executor

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.shutdown()

    # def _dispatch_ready_callbacks(self):
    #     if self._stop_after_user_callback:
    #         self._loop.stop()

    #     ready_entities = self._executor.get_ready_entities()
    #     for entity, number_of_events, callback in self._executor.ready_entities:
    #         callback(self.create_task, entity, number_of_events)

    def _execute_entity(self, callback: Callable[..., Any], *args: Any, **kwargs: Any
                    ) -> asyncio.Task:
        task = self.create_task(callback, args, kwargs)
        task.add_done_callback(self._exception_handler)
        self._tasks.add(task)
        return task

    def create_task(self, callback: Callable[..., Any], *args: Any, **kwargs: Any
                    ) -> asyncio.Task:
        if not asyncio.iscoroutine(callback):
            callback = await_or_execute(callback, *args, **kwargs)
        
        return asyncio.create_task(callback)

    def _exception_handler(self, fut):
        ex = fut.exception()
        if ex:
            raise ex
        
        self._tasks.remove(fut)

    def spin(self):
        self._loop.run_forever()
    
    def spin_once(self, timeout):
        with _timeout(timeout, self._stop_if_running, self._loop):
            self._loop.run_forever()

    def spin_until_future_complete(self, future: asyncio.Future, timeout) -> None:
        with _timeout(timeout, self._stop_if_running, self._loop):
            self._loop.run_until_complete(future)

    def _stop_if_running(self):
        if self._loop.is_running():
            self._loop.stop()

    def spin_once_until_future_complete(self, future: asyncio.Future, timeout):
        self._stop_after_user_callback = True
        self._loop.run_until_complete(future)
        self._stop_after_user_callback = False

    def _set_loop(self) -> None:
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

    def create_future(self):
        if not self._loop:
            raise RuntimeError("No loop is attached to this executor")
        
        return self._loop.create_future()

    def _attach_to_loop(self):
        # self._loop.add_reader(self._executor.fd, self._dispatch_ready_callbacks)
        # self._loop.call_soon(self._executor.update_timers, self._loop)
        ...

    def attach_to_loop(self, loop: asyncio.AbstractEventLoop):
        if self._loop:
            self._detach_from_loop()
        
        self._loop = loop
        self._attach_to_loop()
        
    def _detach_from_loop(self):
        # self._loop.remove_reader(self._executor.fd)
        self._loop = None

    def shutdown(self):
        self._loop.stop()
        self._detach_from_loop()

    def wake(self):
        self._update_entities_from_nodes()

    def add_node(self, node: Node):
        if node in self._nodes:
            return False
        
        self._nodes.add(node)
        node.executor = self
        self._update_entities_from_nodes()
        return True

    def remove_node(self, node: Node):
        if node not in self._nodes:
            return
        
        self._nodes.remove(node)
        self._update_entities_from_nodes()

    def wrap_future(self, rclpy_future: rclpy.Future) -> asyncio.Future:
        asyncio_future = self._loop.create_future()
        _chain_future(rclpy_future, asyncio_future)
        return asyncio_future
    
    def _update_entities_from_nodes(self):
        subscriptions, timers, clients, services, waitables = set(), set(), set(), set(), set()
        for node in self._nodes:
            subscriptions.update(node.subscriptions)
            timers.update(node.timers)
            clients.update(node.clients)
            services.update(node.services)
            waitables.update(node.waitables)
            if getattr(node, 'guards', None):
                raise RuntimeError("Guard conditions not supported")

        # Sync each entity category
        self._update_entity_set(
            self._subscriptions, 
            subscriptions,
            self._add_subscription,
            self.__executor.remove_subscription
        )

    def _make_callback(self, callback: Callable[[], None]) -> Callable[[], None]:
        return partial(self._loop.call_soon_threadsafe, callback)

    def _add_subscription(self, subscription: Subscription):
        callback = partial(self._handle_ready_subscription, subscription)
        self.__executor.add_subscription(
            subscription,
            self._make_callback(callback)
        )
    
    def _handle_ready_subscription(subscription)

    def _update_entity_set(
        self,
        current_set: set,
        new_set: set,
        added_cb: Callable[[Any], None],
        removed_cb: Callable[[Any], None]
    ) -> None:
        # Handle additions
        for h in new_set - current_set:
            current_set.add(h)
            added_cb(h)
            
        # Handle removals
        for h in current_set - new_set:
            current_set.remove(h)
            removed_cb(h)
        
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
