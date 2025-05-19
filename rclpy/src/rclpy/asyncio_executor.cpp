#include "asyncio_executor.hpp"

namespace py = pybind11;

namespace rclpy
{
namespace asyncio_executor
{

AsyncioExecutor::AsyncioExecutor()
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
