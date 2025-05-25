// Copyright 2024-2025 Brad Martin
// Copyright 2024 Merlin Labs, Inc.
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

#ifndef RCLPY__EVENTS_EXECUTOR__SCOPED_WITH_HPP_
#define RCLPY__EVENTS_EXECUTOR__SCOPED_WITH_HPP_

#include <pybind11/pybind11.h>
namespace py = pybind11;
namespace rclpy
{
namespace events_executor
{

/// Enters a python context manager for the scope of this object instance.
class ScopedWith
{
public:
  explicit ScopedWith(py::handle handle)
  : handle_(handle)
  {
    py::gil_scoped_acquire gil_acquire;
    // entities are removed from node before they are removed from executor
    // we must hold reference to the handle to prevent it's destruction
    handle_.inc_ref();
    handle_.attr("__enter__")();
  }

  ~ScopedWith()
  {
    py::gil_scoped_acquire gil_acquire;
    handle_.attr("__exit__")(py::none(), py::none(), py::none());
    handle_.dec_ref();
  }

private:
  py::handle handle_;
};

}  // namespace events_executor
}  // namespace rclpy

#endif  // RCLPY__EVENTS_EXECUTOR__SCOPED_WITH_HPP_
