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

#include "asyncio_executor.hpp"
#include "events_executor/rcl_support.hpp"
#include "pybind11/functional.h"

namespace py = pybind11;

namespace rclpy
{
namespace asyncio_executor
{

AsyncioExecutor::AsyncioExecutor()
{
}

// pybind takes care of converting the python function to a std::function for us and holding it's reference
// the python function is a 
void AsyncioExecutor::add_subscription(py::object subscription, std::function<void(size_t n)> callback)
{
  RegisterEventCallback<
    rcl_subscription_set_on_new_message_callback,
    rcl_subscription_t,
    Subscription>(subscription, std::move(callback));
}

void AsyncioExecutor::remove_subscription(py::object subscription)
{
  ClearEventCallback<
    rcl_subscription_set_on_new_message_callback,
    rcl_subscription_t,
    Subscription>(subscription);
}

// pybind11 module bindings

void define_asyncio_executor(py::object m)
{
  py::class_<AsyncioExecutor>(m, "AsyncioExecutor")
  .def("add_subscription", &AsyncioExecutor::add_subscription)
  .def("remove_subscription", &AsyncioExecutor::remove_subscription);
}

}  // namespace asyncio_executor
}  // namespace rclpy
