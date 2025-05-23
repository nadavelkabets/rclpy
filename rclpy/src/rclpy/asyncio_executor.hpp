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
  explicit AsyncioExecutor(py::object ex);
  ~AsyncioExecutor();

private:
  const void * WrapCallback(const void * key, std::function<void(size_t number_of_events)> callback, std::shared_ptr<ScopedWith> with);
  pybind11::object create_task(
    pybind11::object callback, pybind11::args args = {}, const pybind11::kwargs & kwargs = {});
  py::handle ex_;
  py::handle get_loop_;
  py::handle create_task_;
  void OnWake();
  void CallSoon(std::function<void()> callback);
};

void define_asyncio_executor(pybind11::object module);

}  // namespace asyncio_executor
}  // namespace rclpy

#endif  // RCLPY__ASYNCIO_EXECUTOR_HPP_
