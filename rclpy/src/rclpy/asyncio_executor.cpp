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

void AsyncioExecutor::CallSoon(std::function<void()> callback)
{
  py::gil_scoped_acquire gil_acquire;
  py::handle loop = ex_.attr("get_loop")();
  loop.attr("call_soon_threadsafe")(callback);
}

void AsyncioExecutor::OnWake()
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
