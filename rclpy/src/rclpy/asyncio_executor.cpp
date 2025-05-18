#include "asyncio_executor.hpp"

namespace py = pybind11;

namespace rclpy
{
namespace asyncio_executor
{

AsyncioExecutor::AsyncioExecutor()
{
}

AsyncioExecutor::~AsyncioExecutor() 
{
}

// pybind11 module bindings

void define_asyncio_executor(py::object module)
{
  py::class_<AsyncioExecutor>(module, "AsyncioExecutor");
}

}  // namespace asyncio_executor
}  // namespace rclpy
