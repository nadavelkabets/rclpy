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
import inspect
from functools import partial
from sys import exc_info
import time
from tkinter import SEL_FIRST
import traceback
from typing import (Any, Callable, Coroutine, Dict, Generator, List, Optional, Set,
                    Type, TypeVar, Union)

from rclpy.task import Task
from rclpy.client import Client
from rclpy.clock import ClockChange, JumpHandle, JumpThreshold, ROSClock, TimeJump
from rclpy.constants import S_TO_NS
from rclpy.context import Context
from rclpy.duration import Duration
from rclpy.executors import (await_or_execute, BaseExecutor, ExternalShutdownException,
                             TracebackType)
from rclpy.logging import get_logger
from rclpy.node import Node
from rclpy.service import Service
from rclpy.subscription import Subscription
from rclpy.timer import Timer
from rclpy.utilities import get_default_context

EntityT = TypeVar('EntityT', bound=Union[Subscription, Service, Client, Timer])


class TimerHandler:
    def __init__(
        self,
        timer: Timer,
        loop: asyncio.AbstractEventLoop,
        schedule_callback: Callable,
    ) -> None:
        self._timer = timer
        self._loop = loop
        self._schedule_callback = schedule_callback

        self._call_later_handle: Optional[asyncio.TimerHandle] = None
        self._jump_handle: Optional[JumpHandle] = None

        if self._is_finished():
            return

        self._register_jump_handle()
        if not self._ros_time_is_active():
            self._schedule_next_call()

    def on_remove(self) -> None:
        self._cancel_call_later()
        self._unregister_jump_handle()

    def on_reset(self, _: int = None) -> None:
        self._cancel_call_later()
        self._register_jump_handle()
        if not self._ros_time_is_active():
            self._schedule_next_call()

    def _cancel_call_later(self) -> None:
        if self._call_later_handle:
            self._call_later_handle.cancel()
            self._call_later_handle = None

    def _ros_time_is_active(self) -> bool:
        return isinstance(self._timer.clock, ROSClock) and self._timer.clock.ros_time_is_active

    def _register_jump_handle(self) -> None:
        if not self._jump_handle:
            threshold = JumpThreshold(min_forward=Duration(nanoseconds=1), min_backward=None)
            self._jump_handle = self._timer.clock.create_jump_callback(
                threshold,
                post_callback=self._on_time_jump
            )

    def _unregister_jump_handle(self) -> None:
        if self._jump_handle:
            self._jump_handle.unregister()
            self._jump_handle = None

    def _on_time_jump(self, jump: TimeJump) -> None:
        if self._is_finished():
            return

        if jump.clock_change == ClockChange.ROS_TIME_ACTIVATED:
            self._cancel_call_later()
        elif jump.clock_change == ClockChange.ROS_TIME_DEACTIVATED:
            self._schedule_next_call()
        else:
            self._call_if_ready()

    def _loop_callback(self) -> None:
        if self._is_finished():
            return

        self._call_if_ready()
        self._schedule_next_call()

    def _is_finished(self) -> bool:
        if self._is_timer_destroyed() or self._timer.is_canceled():
            self._cancel_call_later()
            self._unregister_jump_handle()
            return True

        return False

    def _schedule_next_call(self) -> None:
        self._call_later_handle = self._loop.call_later(
            self._time_until_next_call_sec(),
            self._loop_callback
        )

    def _call_if_ready(self) -> float:
        if self._timer.is_ready():
            with self._timer.handle:
                self._timer.handle.call_timer()

            self._schedule_callback(
                partial(await_or_execute, self._timer.callback),
                traceback.print_exception
            )

    def _time_until_next_call_sec(self):
        return self._timer.time_until_next_call() / S_TO_NS 

    def _is_timer_destroyed(self) -> bool:
        return self._timer.handle.pointer == 0


class AsyncioExecutor(BaseExecutor[asyncio.Future, asyncio.Task]):
    def __init__(
        self, loop: Optional[asyncio.AbstractEventLoop] = None,
        *,
        context: Optional[Context] = None
    ) -> None:
        self._loop = loop or self._get_loop()
        self._context = context or get_default_context()
        self._context.on_shutdown(self.shutdown)

        self._ready_tasks: asyncio.Queue = asyncio.Queue()
        self._tasks: Set[Task] = set()
        self._nodes: Set[Node] = set()
        self._subscriptions: Set[Subscription] = set()
        self._clients: Set[Client] = set()
        self._services: Set[Service] = set()
        self._timers: Set[Timer] = set()
        self._timer_handlers: Dict[Timer, TimerHandler] = {}
        self._shutdown_event = asyncio.Event()
        self._shutdown_waiter = self._loop.create_task(self._shutdown_event.wait())

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

    def __enter__(self) -> 'AsyncioExecutor':
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.shutdown()

    def shutdown(self, timeout_sec: Optional[float] = None) -> bool:
        """Clear all nodes and close the event loop."""
        self._shutdown_event.set()
        self._shutdown_waiter.cancel()
        
        self._nodes.clear()
        self._update_entities_from_nodes()

        while not self._ready_tasks.empty():
            self._ready_tasks.get_nowait()

        for _ in range(len(self._tasks)):
            task = self._tasks.pop()
            task.cancel()

        if not self._loop.is_running() and not self._loop.is_closed():
            try:
                self._loop.run_until_complete(
                    asyncio.wait_for(self._shutdown_waiter, timeout=timeout_sec))
            except asyncio.CancelledError:
                pass

        return self._shutdown_waiter.cancelled()

    def __del__(self):
        self.shutdown()
        if not self._loop.is_closed():
            self._loop.close()

    def _resume_task(self, task: Task):
        self._ready_tasks.put_nowait(task)

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    async def spin_async(self, future: Optional[asyncio.Future] = None, timeout_sec: Optional[float] = None):
        timeout = None
        timeout_epoch = time.time() + timeout_sec if timeout_sec is not None else None
        while self.context.ok() and not self._shutdown_event.is_set():
            if timeout_epoch:
                timeout = timeout_epoch - time.time()
            await self.spin_once_async(future=future, timeout_sec=timeout)
            
            if future and (future.done() or future.cancelled()):
                break
            if timeout_epoch and (time.time() > timeout_epoch):
                break

    async def spin_once_async(self, future: Optional[asyncio.Future] = None, timeout_sec: Optional[float] = None):
        if self._shutdown_event.is_set():
            return
        
        ready_task_getter = self._loop.create_task(self._ready_tasks.get())
        futures_to_wait = [self._shutdown_waiter, ready_task_getter]

        if future:
            futures_to_wait.append(future)
            
        done, pending = await asyncio.wait(futures_to_wait, timeout=timeout_sec, return_when=asyncio.FIRST_COMPLETED)

        try:
            if self._shutdown_waiter in done:
                return

            if not self._context.ok():
                raise ExternalShutdownException()
            
            if ready_task_getter in done:
                task = ready_task_getter.result()
                task()

                if task in self._tasks and (task.done() or task.cancelled()):
                    self._tasks.remove(task)

        finally:
            if ready_task_getter in pending:
                ready_task_getter.cancel()
            
            try:
                await ready_task_getter
            except asyncio.CancelledError:
                pass

    def spin(self) -> None:
        self._loop.run_until_complete(self.spin_async())

    def spin_once(self, timeout_sec: Optional[float] = None) -> None:
        self._loop.run_until_complete(self.spin_once_async(timeout_sec=timeout_sec))

    def spin_once_until_future_complete(
        self, future: asyncio.Future, timeout_sec: Optional[float] = None
    ) -> None:
        self._loop.run_until_complete(self.spin_once_async(future=future, timeout_sec=timeout_sec))

    def spin_until_future_complete(
        self, future: asyncio.Future, timeout_sec: Optional[float] = None
    ) -> None:
        self._loop.run_until_complete(self.spin_async(future=future, timeout_sec=timeout_sec))

    def create_task(
        self, callback: Union[Callable, Coroutine], *args: Any, **kwargs: Any
    ) -> Task:
        task = Task(handler=callback, args=args, kwargs=kwargs, executor=self)
        self._ready_tasks.put_nowait(task)
        return task

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
            self._handle_added_timer,
            self._handle_removed_timer,
        )

    def _handle_added_timer(self, timer: Timer):
        handler = TimerHandler(timer, self._loop, self._schedule_ready_callback)
        self._timer_handlers[timer] = handler
        timer.set_on_reset_callback(handler.on_reset)

    def _handle_removed_timer(self, timer: Timer):
        timer.clear_on_reset_callback()
        self._timer_handlers[timer].on_remove()
        del self._timer_handlers[timer]

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

    def _handle_ready_subscription(
            self,
            subscription: Subscription,
            number_of_events: int
    ) -> None:
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

    def _handle_ready_entity(
        self,
        take_entity_callback: Callable[[EntityT], Optional[Coroutine]],
        entity: EntityT,
        number_of_events: int,
    ) -> None:
        for _ in range(number_of_events):
            callback = take_entity_callback(entity)
            if not callback:
                break

            self._schedule_ready_callback(
                callback,
                lambda exc: get_logger(entity.get_logger_name()).error("".join(traceback.format_exception(exc)))
            )

    def _schedule_ready_callback(
        self,
        callback: Callable[[], Coroutine],
        exception_handler: Callable[[Exception], None]
    ) -> None:
        async def wrapped_coroutine():
            try:
                await callback()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                exception_handler(exc)

        self._tasks.add(self.create_task(wrapped_coroutine))
