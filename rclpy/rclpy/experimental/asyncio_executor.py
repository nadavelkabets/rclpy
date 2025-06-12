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
from sys import exc_info, stderr
import time
import traceback
from typing import (Any, Callable, Coroutine, Dict, Generator, List, Optional, Set,
                    Type, TypeVar, Union)

from rclpy.exceptions import NotInitializedException
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
from rclpy.time import Time
from rclpy.task import Future

EntityT = TypeVar('EntityT', bound=Union[Subscription, Service, Client, Timer])


class _WaitHandler:
    def __init__(
        self,
        clock: ROSClock,
        loop: asyncio.AbstractEventLoop,
        is_finished: Callable[[], bool],
        is_ready: Callable[[], bool],
        time_until_ready_sec: Callable[[], float],
        on_ready: Callable[[], None],
    ) -> None:
        self._clock = clock
        self._loop = loop
        self._is_finished = is_finished
        self._is_ready = is_ready
        self._time_until_ready_sec = time_until_ready_sec
        self._on_ready = on_ready
        self._call_later: Optional[asyncio.TimerHandle] = None
        self._jump_handle: Optional[JumpHandle] = None
        self._register_jump_handle()
        self._process()

    def cancel(self) -> None:
        if self._call_later:
            self._call_later.cancel()
            self._call_later = None
        if self._jump_handle:
            self._jump_handle.unregister()
            self._jump_handle = None

    def _process(self) -> None:
        if self._is_finished():
            self.cancel()
            return

        if self._is_ready():
            self._on_ready()
            if not self._is_finished() and not self._ros_time_active():
                self._schedule()

        elif not self._ros_time_active():
            self._schedule()

    def _schedule(self) -> None:
        if self._call_later:
            self._call_later.cancel()
        delay = max(self._time_until_ready_sec(), 0.0)
        self._call_later = self._loop.call_later(delay, self._process)

    def _register_jump_handle(self) -> None:
        threshold = JumpThreshold(min_forward=Duration(nanoseconds=1), min_backward=None)
        self._jump_handle = self._clock.create_jump_callback(threshold, post_callback=self._on_jump)

    def _on_jump(self, jump: TimeJump) -> None:
        if self._is_finished():
            self.cancel()
            return
        if jump.clock_change in (
            ClockChange.ROS_TIME_ACTIVATED,
            ClockChange.ROS_TIME_DEACTIVATED,
        ):
            self.cancel()
            print(
                f"Cancelling callback due to clock change: {jump.clock_change}",
                file=stderr
            )
            return
        self._process()

    def _ros_time_active(self) -> bool:
        return isinstance(self._clock, ROSClock) and self._clock.ros_time_is_active


class _TimerHandler:
    def __init__(self, timer: Timer, loop: asyncio.AbstractEventLoop, schedule_cb) -> None:
        self._timer = timer
        self._schedule_cb = schedule_cb
        self._loop = loop
        self._build_waiter()
        self._timer.set_on_reset_callback(self.on_reset)

    def _build_waiter(self) -> None:
        self._waiter = _WaitHandler(
            clock=self._timer.clock,
            loop=self._loop,
            is_finished=self._finished,
            is_ready=self._ready,
            time_until_ready_sec=self._time_until_ready,
            on_ready=self._on_ready,
        )

    def _finished(self) -> bool:
        return self._timer.handle.pointer == 0 or self._timer.is_canceled()

    def _ready(self) -> bool:
        return self._timer.is_ready()

    def _time_until_ready(self) -> float:
        return self._timer.time_until_next_call() / S_TO_NS

    def _on_ready(self) -> None:
        with self._timer.handle:
            self._timer.handle.call_timer()
        self._schedule_cb(
            partial(await_or_execute, self._timer.callback),
            traceback.print_exception,
        )

    def on_remove(self) -> None:
        self._waiter.cancel()

    def on_reset(self, _: int = None) -> None:
        self._waiter.cancel()
        self._build_waiter()


class _SleepWaiter:
    def __init__(self, clock: ROSClock, until: Time, loop: asyncio.AbstractEventLoop, fut: asyncio.Future) -> None:
        self._clock = clock
        self._until = until
        self._fut = fut
        self._waiter = _WaitHandler(
            clock=clock,
            loop=loop,
            is_finished=self._finished,
            is_ready=self._ready,
            time_until_ready_sec=self._time_until_ready,
            on_ready=self._on_ready,
        )

    def _on_shutdown(self) -> None:
        if not self._fut.done():
            self._fut.set_result(False)
        self._waiter.cancel()

    def _finished(self) -> bool:
        return self._fut.done()

    def _ready(self) -> bool:
        return self._clock.now() >= self._until

    def _time_until_ready(self) -> float:
        return (self._until - self._clock.now()).nanoseconds / S_TO_NS

    def _on_ready(self) -> None:
        if not self._fut.done():
            self._fut.set_result(True)

    def cancel(self) -> None:
        self._waiter.cancel()


class AsyncioClock(ROSClock):
    async def sleep_for_async(self, rel_time: Duration, *, context: Optional[Context] = None) -> bool:
        return await self.sleep_until_async(self.now() + rel_time, context=context)

    async def sleep_until_async(self, until: Time, *, context: Optional[Context] = None) -> bool:
        if until.clock_type != self.clock_type:
            raise ValueError
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        waiter = _SleepWaiter(self, until, loop, fut)
        try:
            return await fut
        finally:
            waiter.cancel()


class AsyncioExecutor(BaseExecutor):
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
        self._timer_handlers: Dict[Timer, _TimerHandler] = {}
        self._shutdown_fut = self._loop.create_future()

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
        if not self._shutdown_fut.done():
            self._shutdown_fut.set_result(None)

        self._nodes.clear()
        self._update_entities_from_nodes()

        while not self._ready_tasks.empty():
            self._ready_tasks.get_nowait()

        for _ in range(len(self._tasks)):
            task = self._tasks.pop()
            task.cancel()

        return True

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
        while self.context.ok() and not self._shutdown_fut.done():
            if timeout_epoch:
                timeout = timeout_epoch - time.time()
            await self.spin_once_async(future=future, timeout_sec=timeout)
            
            if future and (future.done() or future.cancelled()):
                break
            if timeout_epoch and (time.time() > timeout_epoch):
                break

    async def spin_once_async(self, future: Optional[asyncio.Future] = None, timeout_sec: Optional[float] = None):
        if self._shutdown_fut.done():
            return
        
        task = None
        try:
            task = self._ready_tasks.get_nowait()
        except asyncio.QueueEmpty:
            ready_task_getter = self._loop.create_task(self._ready_tasks.get())
            futures_to_wait = [self._shutdown_fut, ready_task_getter]

            if future:
                futures_to_wait.append(future)
                
            done, pending = await asyncio.wait(futures_to_wait, timeout=timeout_sec, return_when=asyncio.FIRST_COMPLETED)

            if ready_task_getter in pending:
                ready_task_getter.cancel()
                try:
                    await ready_task_getter
                except asyncio.CancelledError:
                    return
                return

            if self._shutdown_fut in done:
                return
        
            task = ready_task_getter.result()
        finally:
            if not self._context.ok():
                raise ExternalShutdownException()

        task()
        if task.done() or task.cancelled():
            try:
                self._tasks.remove(task)
            except KeyError:
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
    
    def wrap_future(self, rclpy_future: Future) -> asyncio.Future:
        """
        Chain two futures so that when one completes, so does the other.

        The result (or exception) of source will be copied to destination.
        If destination is cancelled, source gets cancelled too.
        """

        asyncio_future = self._loop.create_future()
        def _call_check_cancel(_: asyncio.Future):
            if asyncio_future.cancelled():
                rclpy_future.cancel()

        def _call_set_state(_: Future):
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
        handler = _TimerHandler(timer, self._loop, self._schedule_ready_callback)
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
