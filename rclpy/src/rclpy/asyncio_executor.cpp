#include "asyncio_executor.hpp"
#include "events_executor/rcl_support.hpp"
#include "pybind11/functional.h"

namespace py = pybind11;

namespace rclpy
{
namespace asyncio_executor
{

AsyncioExecutor::AsyncioExecutor()
{
}

// pybind takes care of converting the python function to a std::function for us and holding it's reference
// the python function is a 
void AsyncioExecutor::add_subscription(py::object subscription, std::function<void(size_t n)> callback)
{
  RegisterEventCallback<
    rcl_subscription_set_on_new_message_callback,
    rcl_subscription_t,
    Subscription>(subscription, std::move(callback));
}

void AsyncioExecutor::remove_subscription(py::object subscription)
{
  ClearEventCallback<
    rcl_subscription_set_on_new_message_callback,
    rcl_subscription_t,
    Subscription>(subscription);
}

// pybind11 module bindings

void define_asyncio_executor(py::object m)
{
  py::class_<AsyncioExecutor>(m, "AsyncioExecutor");
}

}  // namespace asyncio_executor
}  // namespace rclpy
