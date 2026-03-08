import asyncio
from types import TracebackType
from typing import Any, Callable, Optional, Set, Type, Union

from rclpy.clock import ClockChange, JumpThreshold
from rclpy.context import Context
from rclpy.duration import Duration
from rclpy.exceptions import TimeSourceChangedError
from rclpy.node import BaseNode
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile, qos_profile_rosout_default, qos_profile_services_default
from rclpy.subscription_content_filter_options import ContentFilterOptions
from rclpy.type_support import MsgT, Srv, SrvRequestT, SrvResponseT

from .async_client import AsyncClient
from .async_publisher import AsyncPublisher
from .async_service import AsyncService
from .async_subscription import AsyncSubscription


class AsyncNode(BaseNode):
    def __init__(
        self,
        node_name: str,
        *,
        context: Optional[Context] = None,
        cli_args: Optional[list[str]] = None,
        namespace: Optional[str] = None,
        use_global_arguments: bool = True,
        enable_rosout: bool = True,
        rosout_qos_profile: Union[QoSProfile, int] = qos_profile_rosout_default,
        start_parameter_services: bool = True,
        parameter_overrides: Optional[list[Parameter[Any]]] = None,
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
        self._publishers: Set[AsyncPublisher] = set()
        self._subscriptions: Set[AsyncSubscription] = set()
        self._services: Set[AsyncService] = set()
        self._clients: Set[AsyncClient] = set()
        self._pending_sleeps: Set[asyncio.Future] = set()

    async def __aenter__(self) -> 'AsyncNode':
        self.handle.__enter__()
        tg = asyncio.TaskGroup()
        self._tg = await tg.__aenter__()
        self._setup()
        # TODO: Add TypeDescriptionService support for AsyncNode
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        tg = self._tg
        self._tg = None
        self._pending_sleeps = None
        try:
            await tg.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self.handle.__exit__(exc_type, exc_val, exc_tb)
            self.handle.destroy_when_not_in_use()

    async def close(self) -> None:
        for future in self._pending_sleeps:
            future.cancel()
        async with asyncio.TaskGroup() as tg:
            for pub in self._publishers:
                tg.create_task(pub.close())
            for sub in self._subscriptions:
                tg.create_task(sub.close())
            for srv in self._services:
                tg.create_task(srv.close())
            for cli in self._clients:
                tg.create_task(cli.close())

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

    async def _run_entity(self, entity: Any, entity_set: set) -> None:
        try:
            await entity._run()
        finally:
            entity_set.discard(entity)

    def create_publisher(
        self,
        msg_type: Type[MsgT],
        topic: str,
        qos_profile: Union[QoSProfile, int],
    ) -> AsyncPublisher[MsgT]:
        if self._tg is None:
            raise RuntimeError("Node context manager not active")
        qos_profile = self._validate_qos_or_depth_parameter(qos_profile)

        publisher_handle = self._create_publisher_handle(
            msg_type, topic, qos_profile)

        pub = AsyncPublisher(publisher_handle, msg_type, topic, qos_profile)
        self._publishers.add(pub)
        pub._task = self._tg.create_task(self._run_entity(pub, self._publishers))
        return pub

    def create_subscription(
        self,
        msg_type: Type[MsgT],
        topic: str,
        callback: Callable,
        qos_profile: Union[QoSProfile, int],
        *,
        raw: bool = False,
        concurrent: bool = False,
        content_filter_options: Optional[ContentFilterOptions] = None,
    ) -> AsyncSubscription[MsgT]:
        if self._tg is None:
            raise RuntimeError("Node context manager not active")
        qos_profile = self._validate_qos_or_depth_parameter(qos_profile)

        subscription_handle = self._create_subscription_handle(
            msg_type, topic, qos_profile,
            content_filter_options=content_filter_options)

        sub = AsyncSubscription(
            subscription_handle, msg_type, topic, callback,
            qos_profile, raw, concurrent)
        self._subscriptions.add(sub)
        sub._task = self._tg.create_task(self._run_entity(sub, self._subscriptions))
        return sub

    def create_service(
        self,
        srv_type: Type[Srv[SrvRequestT, SrvResponseT]],
        srv_name: str,
        callback: Callable[[SrvRequestT, SrvResponseT], SrvResponseT],
        *,
        qos_profile: QoSProfile = qos_profile_services_default,
        concurrent: bool = False,
    ) -> AsyncService[SrvRequestT, SrvResponseT]:
        if self._tg is None:
            raise RuntimeError("Node context manager not active")

        service_handle = self._create_service_handle(
            srv_type, srv_name, qos_profile=qos_profile)

        srv = AsyncService(
            service_handle, srv_type, srv_name, callback,
            qos_profile, concurrent)
        self._services.add(srv)
        srv._task = self._tg.create_task(self._run_entity(srv, self._services))
        return srv

    def create_client(
        self,
        srv_type: Type[Srv[SrvRequestT, SrvResponseT]],
        srv_name: str,
        *,
        qos_profile: QoSProfile = qos_profile_services_default,
    ) -> AsyncClient[SrvRequestT, SrvResponseT]:
        if self._tg is None:
            raise RuntimeError("Node context manager not active")

        client_handle = self._create_client_handle(
            srv_type, srv_name, qos_profile=qos_profile)

        client = AsyncClient(
            client_handle, srv_type, srv_name, qos_profile)
        self._clients.add(client)
        client._task = self._tg.create_task(self._run_entity(client, self._clients))
        return client
