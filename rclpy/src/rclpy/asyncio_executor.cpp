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

void EventsExecutor::wake()
{
  if (!wake_pending_.exchange(true)) {
    // Update tracked entities.
    
    loop_.attr("call_soon")([this](){this::UpdateEntitiesFromNodes()})
  }
}

// pybind11 module bindings

void define_asyncio_executor(py::object m)
{
  py::class_<AsyncioExecutor>(m, "AsyncioExecutor")
  .def(py::init<>())
  .def_readwrite("_loop", &AsyncioExecutor::loop_);
}

}  // namespace asyncio_executor
}  // namespace rclpy
