#include "asyncio_executor.hpp"
#include "events_executor/rcl_support.hpp"

namespace py = pybind11;

namespace rclpy
{
namespace asyncio_executor
{

AsyncioExecutor::AsyncioExecutor(py::handle ex)
: ex_(ex)
{
}

void AsyncioExecutor::on_wake()
{
  UpdateEntitiesFromNodes();
}

// pybind11 module bindings

void define_asyncio_executor(py::object m)
{
  py::class_<AsyncioExecutor>(m, "AsyncioExecutor");
}

}  // namespace asyncio_executor
}  // namespace rclpy
