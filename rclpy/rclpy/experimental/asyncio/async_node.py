import asyncio
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union

from rclpy.client import Client
from rclpy.clock import ClockChange, JumpThreshold
from rclpy.context import Context
from rclpy.duration import Duration
from rclpy.executors import await_or_execute
from rclpy.node import BaseNode
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile
from rclpy.service import Service
from rclpy.subscription import Subscription, SubscriptionCallbackUnion
from rclpy.type_support import MsgT, Srv, SrvRequestT, SrvResponseT
from rclpy.exceptions import TimeSourceChangedError
from rclpy.qos import qos_profile_rosout_default
from .async_client import AsyncClient
from .async_service import AsyncService
from .async_subscription import AsyncSubscription

Entity = Union[Subscription, Service, Client]

class AsyncNode(BaseNode):
    def __init__(
        self,
        node_name: str,
        *,
        context: Optional[Context] = None,
        cli_args: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        use_global_arguments: bool = True,
        enable_rosout: bool = True,
        rosout_qos_profile: Union[QoSProfile, int] = qos_profile_rosout_default,
        start_parameter_services: bool = True,
        parameter_overrides: Optional[List[Parameter[Any]]] = None,
        allow_undeclared_parameters: bool = False,
        automatically_declare_parameters_from_overrides: bool = False,
        enable_logger_service: bool = False
    ) -> None:
        super().__init__(
            node_name=node_name,
            context=context,
            cli_args=cli_args,
            namespace=namespace,
            use_global_arguments=use_global_arguments,
            enable_rosout=enable_rosout,
            rosout_qos_profile=rosout_qos_profile,
            start_parameter_services=start_parameter_services,
            parameter_overrides=parameter_overrides,
            allow_undeclared_parameters=allow_undeclared_parameters,
            automatically_declare_parameters_from_overrides=automatically_declare_parameters_from_overrides,
            enable_logger_service=enable_logger_service,
        )
        self._tg: Optional[asyncio.TaskGroup] = None
        self._runners: Dict[Entity, asyncio.Task] = {}
        self._pending_sleeps: Set[asyncio.Future] = set()
        self._subscriptions: Set[AsyncSubscription] = set()
        self._clients: Set[AsyncClient] = set()
        self._services: Set[AsyncService] = set()


    async def __aenter__(self) -> 'AsyncNode':
        self.handle.__enter__()
        tg = asyncio.TaskGroup()
        self._tg = await tg.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        tg = self._tg
        self._tg = None
        self._runners = None
        self._pending_sleeps = None
        try:
            await tg.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self.handle.__exit__()
            self.handle.destroy_when_not_in_use()

    async def close(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for sub in list(self._subscriptions):
                tg.create_task(sub.close())
            for cli in self._clients:
                tg.create_task(cli.close())
            for srv in self._services:
                tg.create_task(srv.close())

    async def sleep(self, duration_sec: float) -> None:
        """
        Sleep for a duration respecting sim time.

        Cancelled on close(). Raises TimeSourceChangedError if ROS time is
        activated or deactivated during the sleep.
        """
        if self._pending_sleeps is None:
            raise RuntimeError("sleep() requires the node context manager to be active")
        if duration_sec <= 0:
            return

        clock = self.get_clock()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        timer_handle = None
        target = None

        def _resolve() -> None:
            if not future.done():
                future.set_result(None)

        def _reject() -> None:
            if not future.done():
                future.set_exception(TimeSourceChangedError())

        if clock.ros_time_is_active:
            target = clock.now() + Duration(nanoseconds=int(duration_sec * 1e9))
        else:
            timer_handle = loop.call_later(duration_sec, _resolve)

        def _on_jump(time_jump: Any) -> None:
            if time_jump.clock_change in (
                ClockChange.ROS_TIME_ACTIVATED,
                ClockChange.ROS_TIME_DEACTIVATED,
            ):
                loop.call_soon_threadsafe(_reject)
            elif target is not None and clock.now() >= target:
                loop.call_soon_threadsafe(_resolve)

        threshold = JumpThreshold(
            min_forward=Duration(nanoseconds=1),
            min_backward=None,
            on_clock_change=True,
        )
        jump_handle = clock.create_jump_callback(
            threshold, post_callback=_on_jump)
        self._pending_sleeps.add(future)
        try:
            await future
        finally:
            self._pending_sleeps.discard(future)
            jump_handle.unregister()
            if timer_handle is not None:
                timer_handle.cancel()

    # TODO: do we want a concurrent=False flag that awaits the callback?
    # actually we could achieve this with a max_concurrent semaphore limited to 1
    # TODO: do we want to utilize asyncio's eager_start on 3.12+?
    async def _run_subscription(self, subscription: Subscription[MsgT]) -> None:
        """Node owns the DDS bridge and read loop for subscriptions."""
        loop = asyncio.get_running_loop()
        read_event = asyncio.Event()

        def _on_new_message(_num_waiting):
            loop.call_soon_threadsafe(read_event.set)

        with subscription.handle:
            subscription.handle.set_on_new_message_callback(_on_new_message)
            try:
                async with asyncio.TaskGroup() as tg:
                    while True:
                        # TODO: share code with executors.py _take_subscription
                        msg_and_info = subscription.handle.take_message(
                            subscription.msg_type, subscription.raw)
                        if msg_and_info is not None:
                            if subscription._callback_type is Subscription.CallbackType.MessageOnly:
                                msg_tuple = (msg_and_info[0],)
                            else:
                                msg_tuple = msg_and_info
                            tg.create_task(await_or_execute(
                                subscription.callback, *msg_tuple))
                        else:
                            try:
                                read_event.clear()
                                await read_event.wait()
                            except asyncio.CancelledError:
                                break  # let in-flight callbacks complete
            finally:
                subscription.handle.clear_on_new_message_callback()
                Node.destroy_subscription(self, subscription)

    async def _run_service(self, service: Service[SrvRequestT, SrvResponseT]) -> None:
        """Node owns the DDS bridge and read loop for services."""
        loop = asyncio.get_running_loop()
        read_event = asyncio.Event()

        def _on_new_request(_num_waiting):
            loop.call_soon_threadsafe(read_event.set)

        with service.handle:
            service.handle.set_on_new_request_callback(_on_new_request)
            try:
                async with asyncio.TaskGroup() as tg:
                    while True:
                        # TODO: share code with executors.py _take_service
                        request_and_header = service.handle.service_take_request(
                            service.srv_type.Request)
                        if request_and_header != (None, None):
                            tg.create_task(self._handle_service_request(
                                service, request_and_header[0], request_and_header[1]))
                        else:
                            try:
                                read_event.clear()
                                await read_event.wait()
                            except asyncio.CancelledError:
                                break  # let in-flight callbacks complete
            finally:
                service.handle.clear_on_new_request_callback()
                Node.destroy_service(self, service)

    async def _handle_service_request(
        self,
        service: Service[SrvRequestT, SrvResponseT],
        request: SrvRequestT,
        header: Any,
    ) -> None:
        response = await await_or_execute(
            service.callback, request, service.srv_type.Response())
        service.send_response(response, header)

    async def _run_client(self, client: Client[SrvRequestT, SrvResponseT]) -> None:
        """Node owns the DDS bridge and response routing for clients."""
        loop = asyncio.get_running_loop()
        read_event = asyncio.Event()

        def _on_new_response(_num_waiting):
            loop.call_soon_threadsafe(read_event.set)

        with client.handle:
            client.handle.set_on_new_response_callback(_on_new_response)
            try:
                while True:
                    # TODO: share code with executors.py _take_client
                    header_and_response = client.handle.take_response(
                        client.srv_type.Response)
                    if header_and_response != (None, None):
                        header, response = header_and_response
                        future = client._pending_requests.get(
                            header.request_id.sequence_number)
                        if future is not None:
                            future.set_result(response)
                    else:
                        try:
                            read_event.clear()
                            await read_event.wait()
                        except asyncio.CancelledError:
                            break
            finally:
                client.handle.clear_on_new_response_callback()
                for future in client._pending_requests.values():
                    future.cancel()
                client._pending_requests.clear()
                Node.destroy_client(self, client)

    def create_subscription(
        self,
        msg_type: Type[MsgT],
        topic: str,
        callback: SubscriptionCallbackUnion[MsgT],
        qos_profile: Union[QoSProfile, int],
        **kwargs: Any,
    ) -> Subscription[MsgT]:
        sub = super().create_subscription(msg_type, topic, callback, qos_profile, **kwargs)
        if self._tg:
            task = self._tg.create_task(self._run_subscription(sub))
            self._runners[sub] = task
        return sub

    def create_service(
        self,
        srv_type: Type[Srv],
        srv_name: str,
        callback: Callable[[SrvRequestT, SrvResponseT], SrvResponseT],
        **kwargs: Any,
    ) -> Service[SrvRequestT, SrvResponseT]:
        srv = super().create_service(srv_type, srv_name, callback, **kwargs)
        if self._tg:
            task = self._tg.create_task(self._run_service(srv))
            self._runners[srv] = task
        return srv

    def create_client(
        self,
        srv_type: Type[Srv],
        srv_name: str,
        **kwargs: Any,
    ) -> Client[SrvRequestT, SrvResponseT]:
        client = super().create_client(srv_type, srv_name, **kwargs)

        # TODO: replace the monkeypatch with AsyncioClient class
        async def call(request: SrvRequestT) -> SrvResponseT:
            future: asyncio.Future[SrvResponseT] = asyncio.get_running_loop().create_future()
            sequence_number = client.handle.send_request(request)
            client._pending_requests[sequence_number] = future
            try:
                return await future
            finally:
                client._pending_requests.pop(sequence_number, None)

        client.call = call

        if self._tg:
            task = self._tg.create_task(self._run_client(client))
            self._runners[client] = task
        return client

    async def close_subscription(self, sub: Subscription[MsgT]) -> None:
        """Stop processing new messages and wait for existing callbacks to complete."""
        task = self._runners.pop(sub, None) if self._runners is not None else None
        if task is not None:
            task.cancel()
            await task
        else:
            Node.destroy_subscription(self, sub)

    async def close_service(self, srv: Service[SrvRequestT, SrvResponseT]) -> None:
        """Stop processing new requests and wait for existing callbacks to complete."""
        task = self._runners.pop(srv, None) if self._runners is not None else None
        if task is not None:
            task.cancel()
            await task
        else:
            Node.destroy_service(self, srv)

    async def close_client(self, client: Client[SrvRequestT, SrvResponseT]) -> None:
        """Stop allowing new calls and cancel all pending calls."""
        task = self._runners.pop(client, None) if self._runners is not None else None
        if task is not None:
            task.cancel()
            await task
        else:
            Node.destroy_client(self, client)
