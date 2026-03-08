# Copyright 2018 Open Source Robotics Foundation, Inc.
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

from typing import List, Set, TYPE_CHECKING

from rcl_interfaces.msg import SetParametersResult
from rclpy.clock import ROSClock
from rclpy.clock_type import ClockType
from rclpy.parameter import Parameter
from rclpy.qos import QoSProfile
from rclpy.qos import ReliabilityPolicy
from rclpy.time import Time
import rosgraph_msgs.msg

if TYPE_CHECKING:
    from rclpy.node import BaseNode

CLOCK_TOPIC = '/clock'
USE_SIM_TIME_NAME = 'use_sim_time'


class TimeSource:

    def __init__(self, node: 'BaseNode'):
        self._associated_clocks: Set[ROSClock] = set()
        # Zero time is a special value that means time is uninitialzied
        self._last_time_set = Time(clock_type=ClockType.ROS_TIME)
        self._ros_time_is_active = False
        self._logger = node.get_logger()

        if not node.has_parameter(USE_SIM_TIME_NAME):
            node.declare_parameter(USE_SIM_TIME_NAME, False)

        use_sim_time_param = node.get_parameter(USE_SIM_TIME_NAME)
        if use_sim_time_param.type_ != Parameter.Type.NOT_SET:
            if use_sim_time_param.type_ == Parameter.Type.BOOL:
                self.ros_time_is_active = use_sim_time_param.value
            else:
                self._logger.error(
                    "Invalid type for parameter '{}' {!r} should be bool"
                    .format(USE_SIM_TIME_NAME, use_sim_time_param.type_))
        else:
            self._logger.debug(
                "'{}' parameter not set, using wall time by default"
                .format(USE_SIM_TIME_NAME))

        node.add_on_set_parameters_callback(self._on_parameter_event)
        node.create_subscription(
            rosgraph_msgs.msg.Clock,
            CLOCK_TOPIC,
            self.clock_callback,
            QoSProfile(depth=1, reliability=ReliabilityPolicy.BEST_EFFORT))

    @property
    def ros_time_is_active(self) -> bool:
        return self._ros_time_is_active

    @ros_time_is_active.setter
    def ros_time_is_active(self, enabled: bool) -> None:
        if self._ros_time_is_active == enabled:
            return
        self._ros_time_is_active = enabled
        for clock in self._associated_clocks:
            clock._set_ros_time_is_active(enabled)

    def attach_clock(self, clock: ROSClock) -> None:
        if not isinstance(clock, ROSClock):
            raise ValueError('Only clocks with type ROS_TIME can be attached.')

        clock.set_ros_time_override(self._last_time_set)
        clock._set_ros_time_is_active(self.ros_time_is_active)
        self._associated_clocks.add(clock)

    async def clock_callback(self, msg: rosgraph_msgs.msg.Clock) -> None:
        if not self._ros_time_is_active:
            return
        # Cache the last message in case a new clock is attached.
        time_from_msg = Time.from_msg(msg.clock)
        self._last_time_set = time_from_msg
        for clock in self._associated_clocks:
            clock.set_ros_time_override(time_from_msg)

    def _on_parameter_event(self, parameter_list: List[Parameter[bool]]) -> SetParametersResult:
        successful = True
        reason = ''

        for parameter in parameter_list:
            if parameter.name == USE_SIM_TIME_NAME:
                if parameter.type_ == Parameter.Type.BOOL:
                    self.ros_time_is_active = parameter.value
                else:
                    successful = False
                    reason = '{} parameter set to something besides a bool'.format(
                        USE_SIM_TIME_NAME)
                    self._logger.error(reason)
                break

        return SetParametersResult(successful=successful, reason=reason)
