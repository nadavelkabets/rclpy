#ifndef RCLPY__ASYNCIO_EXECUTOR_HPP_
#define RCLPY__ASYNCIO_EXECUTOR_HPP_

#include <pybind11/pybind11.h>
#include "events_executor/events_executor.hpp"

namespace rclpy
{
namespace asyncio_executor
{

class AsyncioExecutor : public events_executor::EventsExecutorBase
{
public:
  explicit AsyncioExecutor(py::handle ex);
  ~AsyncioExecutor();

private:
  py::handle ex_;
  void on_wake();
};

void define_asyncio_executor(pybind11::object module);

}  // namespace asyncio_executor
}  // namespace rclpy

#endif  // RCLPY__ASYNCIO_EXECUTOR_HPP_
