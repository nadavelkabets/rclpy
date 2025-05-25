// Copyright 2025 Nadav Elkabets
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

#ifndef RCLPY__ASYNCIO_EXECUTOR_HPP_
#define RCLPY__ASYNCIO_EXECUTOR_HPP_

#include <pybind11/pybind11.h>
#include "events_executor/events_executor.hpp"
#include "events_executor/scoped_with.hpp"

namespace rclpy
{
using namespace events_executor;
namespace asyncio_executor
{

class AsyncioExecutor : public EventsExecutorBase
{
public:
  explicit AsyncioExecutor();
  ~AsyncioExecutor();
  void add_subscription(py::object subscription, std::function<void(size_t n)> callback);
  void remove_subscription(py::object subscription);
};

void define_asyncio_executor(pybind11::object module);

}  // namespace asyncio_executor
}  // namespace rclpy

#endif  // RCLPY__ASYNCIO_EXECUTOR_HPP_
