#include "callbackable.hpp"

namespace rclpy {
extern "C" void RclPyCallbackTrampoline(const void * user_data, size_t number_of_events)
{
    py::gil_scoped_acquire gil_acquire;
    const auto cb = static_cast<const py::object *>(user_data);
    (*cb)(number_of_events);
}

} // namespace rclpy
