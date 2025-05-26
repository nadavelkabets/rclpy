#include "observable.hpp"

namespace rclpy {

extern "C" void RclPyCallbackTrampoline(const void * user_data, size_t number_of_events)
{
    py::gil_scoped_acquire gil_acquire;
    auto * container = static_cast<const CallbackContainer *>(user_data);
    if (container->cb)
    {
       (*container->cb)(number_of_events);
    }
}

void define_abstract_observable(py::object module)
{
  py::class_<ObservableInterface, std::shared_ptr<ObservableInterface>>(module, "Observable")
  .def("set_callback", &ObservableInterface::set_callback)
  .def("clear_callback", &ObservableInterface::clear_callback);
}

} // namespace rclpy


