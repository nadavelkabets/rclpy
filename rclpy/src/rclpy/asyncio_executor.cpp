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

const void * AsyncioExecutor::WrapCallback(const void * key, std::function<void(size_t n)> callback, std::shared_ptr<ScopedWith> with)
{
  std::function<void(size_t n)> cb = [this, callback, with](size_t number_of_events){
    py::gil_scoped_acquire gil_acquire;
    callback(number_of_events);
  };
  return rcl_callback_manager_.MakeCallback(key, std::move(cb), with);
}

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
