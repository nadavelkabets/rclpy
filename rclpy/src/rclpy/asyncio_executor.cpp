#include "asyncio_executor.hpp"
#include "events_executor/rcl_support.hpp"

namespace py = pybind11;

namespace rclpy
{
namespace asyncio_executor
{

AsyncioExecutor::AsyncioExecutor(py::object ex)
: ex_(ex),
  get_loop_(ex_.attr("get_loop")),
  create_task_(ex.attr("create_task"))
{
}

const void * AsyncioExecutor::WrapCallback(const void * key, std::function<void(size_t number_of_events)> callback, std::shared_ptr<ScopedWith> with)
{
  std::function<void(size_t number_of_events)> cb = [this, callback, with](size_t number_of_events){CallSoon(
    // Capturing 'with' here ensures that the subscription object stays alive until the task executes
    [this, callback, number_of_events, with](){callback(number_of_events);}
  );};
  return rcl_callback_manager_.MakeCallback(key, std::move(cb), with);
}

void AsyncioExecutor::CallSoon(std::function<void()> callback)
{
  py::gil_scoped_acquire gil_acquire;
  py::handle loop = get_loop_();
  loop.attr("call_soon_threadsafe")(callback);
}


pybind11::object AsyncioExecutor::create_task(
    pybind11::object callback, pybind11::args args, const pybind11::kwargs & kwargs)
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
