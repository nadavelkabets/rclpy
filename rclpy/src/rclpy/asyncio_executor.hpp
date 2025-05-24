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
