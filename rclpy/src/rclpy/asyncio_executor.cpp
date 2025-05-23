#include "asyncio_executor.hpp"
#include "events_executor/rcl_support.hpp"

namespace py = pybind11;

namespace rclpy
{
namespace asyncio_executor
{

AsyncioExecutor::AsyncioExecutor(py::object get_loop, py::object create_task)
: get_loop_(get_loop),
  create_task_(create_task)
{
}

void AsyncioExecutor::CallSoon(std::function<void()> callback)
{
  py::gil_scoped_acquire gil_acquire;
  py::handle loop = get_loop_();
  loop.attr("call_soon_threadsafe")(callback);
}

pybind11::object AsyncioExecutor::create_task(
    pybind11::object callback, pybind11::args args = {}, const pybind11::kwargs & kwargs = {})
{
  return create_task_(callback, args, kwargs);
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
