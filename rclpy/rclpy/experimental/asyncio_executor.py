from types import TracebackType
from typing import Callable, Optional, Type
from rclpy.executors import Executor
import asyncio
from rclpy.impl.implementation_singleton import rclpy_implementation as _rclpy
import time
from contextlib import contextmanager
import rclpy

@contextmanager
def timeout(timeout: int, callback: Callable[[], None], loop: asyncio.AbstractEventLoop):
    handle = None
    if timeout:
        handle = loop.call_later(timeout, callback)
    yield

    if handle and handle.when() < time.time():
        handle.cancel()


class AsyncioExecutor(Executor):
    def __init__(self, loop=None):
        self._executor = _rclpy.AsyncioExecutor()
        self._tasks = set()
        self._stop_after_user_callback = False
        self._loop: Optional[asyncio.AbstractEventLoop] = self.attach_to_loop(loop)

    def get_loop(self):
        return self._loop

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.shutdown()

    def _dispatch_ready_callbacks(self):
        if self._stop_after_user_callback:
            self._loop.stop()

        ready_entities = self._executor.get_ready_entities()
        for entity, number_of_events, callback in self._executor.ready_entities:
            callback(self.create_task, entity, number_of_events)

    def _add_callback(self, cb):
        task = self.create_task(cb)
        task.add_done_callback(self._exception_handler)
        self._tasks.add(task)
    
    def create_task(self, coro):
        return self._loop.create_task(coro)

    def _exception_handler(self, fut):
        ex = fut.exception()
        if ex:
            raise ex
        
        self._tasks.remove(fut)

    def spin(self):
        self._loop.run_forever()
    
    def spin_once(self, timeout):
        with timeout(timeout, self._stop_if_running, self._loop):
            self._loop.run_forever()

    def spin_until_future_complete(self, future: asyncio.Future, timeout) -> None:
        with timeout(timeout, self._stop_if_running, self._loop):
            self._loop.run_until_complete(future)

    def _stop_if_running(self):
        if self._loop.is_running():
            self._loop.stop()

    def spin_once_until_future_complete(self, future: asyncio.Future, timeout):
        self._stop_after_user_callback = True
        self._loop.run_until_complete(future)
        self._stop_after_user_callback = False

    def _set_event_loop(self, loop=None):
        if loop:
            self._loop = loop
            return
        
        if self._loop:
            return
        
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

    def create_future(self):
        if not self._loop:
            raise RuntimeError("No loop is attached to this executor")
        
        return self._loop.create_future()

    def attach_to_loop(self, loop=None):
        #TODO: allow to reattach to a different loop
        self._set_event_loop(loop)
        self._loop.add_reader(self._executor.fd, self._dispatch_ready_callbacks)
        self._loop.call_soon(self._executor.update_timers, self._loop)

    def shutdown(self):
        self._loop.stop()
        self._loop.remove_reader(self._executor.fd)
        self._loop = None    
