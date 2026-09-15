// Copyright 2026 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RCLPY__WAKEUP_SOCKET_HPP_
#define RCLPY__WAKEUP_SOCKET_HPP_

#include <cstddef>
#include <cstdint>

namespace rclpy
{
/// No socket attached. Equal to INVALID_SOCKET on Windows and to (uintptr_t)-1 on POSIX.
constexpr std::uintptr_t kInvalidWakeupSocket = UINTPTR_MAX;

/// rmw event callback that writes one byte to the socket encoded in user_data.
/**
 * Takes no GIL and no lock, so a middleware thread holding its own locks never blocks here.
 * A full socket buffer means a wakeup is already pending, so that write is dropped.
 */
extern "C" void WakeupSocketTrampoline(const void * user_data, size_t number_of_events);

/// Encode a socket handle as rcl user_data. On macOS this also sets SO_NOSIGPIPE.
const void * wakeup_socket_user_data(std::uintptr_t handle);

/// Close a socket handle that was passed to wakeup_socket_user_data().
void close_wakeup_socket(std::uintptr_t handle);
}  // namespace rclpy

#endif  // RCLPY__WAKEUP_SOCKET_HPP_
