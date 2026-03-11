import asyncio
from types import TracebackType
from typing import Any, Awaitable, Callable, Optional, Set, Type, Union

from rclpy.clock_type import ClockType
from rclpy.context import Context
from rclpy.node import BaseNode
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile, qos_profile_rosout_default, qos_profile_services_default
from rclpy.subscription import AsyncGenericSubscriptionCallback
from rclpy.subscription_content_filter_options import ContentFilterOptions
from rclpy.timer import AsyncTimerCallbackType
from rclpy.type_support import MsgT, Srv, SrvRequestT, SrvResponseT

from .async_client import AsyncClient
from .async_clock import AsyncClock
from .async_publisher import AsyncPublisher
from .async_service import AsyncService
from .async_subscription import AsyncSubscription
from .async_timer import AsyncTimer


class AsyncNode(BaseNode):
    """
    Async context manager node. Must be used with `async with`::

        with rclpy.init():
            async with AsyncNode('my_node') as node:
                node.create_subscription(topic, MsgType, callback, qos)
                node.destroy_node()
    """

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
        self._clock = AsyncClock(clock_type=ClockType.ROS_TIME)
        self._tg: Optional[asyncio.TaskGroup] = None
        self._publishers: Set[AsyncPublisher] = set()
        self._subscriptions: Set[AsyncSubscription] = set()
        self._services: Set[AsyncService] = set()
        self._clients: Set[AsyncClient] = set()
        self._timers: Set[AsyncTimer] = set()
        self._destroyed = False
        # Register with context for rclpy.shutdown() safety net
        self._context.track_node(self)

    async def __aenter__(self) -> 'AsyncNode':
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
        try:
            await self._tg.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self._tg = None
            self.destroy_node()

    def get_clock(self) -> AsyncClock:
        """Get the async clock used by the node."""
        return self._clock

    def destroy_node(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        self._context.untrack_node(self)
        for pub in self._publishers:
            pub.destroy()
        for sub in self._subscriptions:
            sub.destroy()
        for srv in self._services:
            srv.destroy()
        for cli in self._clients:
            cli.destroy()
        for tmr in self._timers:
            tmr.destroy()
        self._clock._destroy()
        self.handle.destroy_when_not_in_use()

    async def _run_entity(self, entity: Any, entity_set: set) -> None:
        try:
            await entity._run()
        finally:
            entity_set.discard(entity)
            entity.destroy()

    def create_publisher(
        self,
        msg_type: Type[MsgT],
        topic: str,
        qos_profile: Union[QoSProfile, int],
    ) -> AsyncPublisher[MsgT]:
        if self._tg is None:
            raise RuntimeError("Cannot create publisher before entering 'async with AsyncNode():'")
        if self._destroyed:
            raise RuntimeError("Cannot create publisher on a destroyed node")
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
        callback: AsyncGenericSubscriptionCallback[MsgT],
        qos_profile: Union[QoSProfile, int],
        *,
        raw: bool = False,
        concurrent: bool = False,
        content_filter_options: Optional[ContentFilterOptions] = None,
    ) -> AsyncSubscription[MsgT]:
        if self._tg is None:
            raise RuntimeError("Cannot create subscription before entering 'async with AsyncNode():'")
        if self._destroyed:
            raise RuntimeError("Cannot create subscription on a destroyed node")
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
        callback: Callable[[SrvRequestT, SrvResponseT], Awaitable[SrvResponseT]],
        *,
        qos_profile: QoSProfile = qos_profile_services_default,
        concurrent: bool = False,
    ) -> AsyncService[SrvRequestT, SrvResponseT]:
        if self._tg is None:
            raise RuntimeError("Cannot create service before entering 'async with AsyncNode():'")
        if self._destroyed:
            raise RuntimeError("Cannot create service on a destroyed node")

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
            raise RuntimeError("Cannot create client before entering 'async with AsyncNode():'")
        if self._destroyed:
            raise RuntimeError("Cannot create client on a destroyed node")

        client_handle = self._create_client_handle(
            srv_type, srv_name, qos_profile=qos_profile)

        client = AsyncClient(
            client_handle, srv_type, srv_name, qos_profile)
        self._clients.add(client)
        client._task = self._tg.create_task(self._run_entity(client, self._clients))
        return client

    def create_timer(
        self,
        timer_period_sec: float,
        callback: AsyncTimerCallbackType,
    ) -> AsyncTimer:
        if self._tg is None:
            raise RuntimeError("Cannot create timer before entering 'async with AsyncNode():'")
        if self._destroyed:
            raise RuntimeError("Cannot create timer on a destroyed node")

        timer_period_ns = int(float(timer_period_sec) * 1e9)
        timer = AsyncTimer(timer_period_ns, self._clock, self.context, callback)
        self._timers.add(timer)
        timer._task = self._tg.create_task(self._run_entity(timer, self._timers))
        return timer
