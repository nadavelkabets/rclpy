#include "pybind11/pybind11.h"
#include "rcl/rcl.h"

namespace py = pybind11;

namespace rclpy {

extern "C" void RclPyCallbackTrampoline(const void * user_data, size_t number_of_events);

template <class RclEntityT>
class Callbackable {
public:
    virtual ~Callbackable()
    {   
        if (callback_)
        {
            py::gil_scoped_acquire gil_acquire;
            clear_callback();
        }
    }
    void set_callback(py::object callback)
    {
        if (callback_)
        {
            clear_callback();
        }

        callback_ = std::make_shared<py::object>(callback);
        SetCallback(rcl_ptr(), RclPyCallbackTrampoline, callback_.get());
    }

    void clear_callback()
    {
        SetCallback(rcl_ptr(), nullptr, nullptr);
        callback_.reset();

    }
    virtual rcl_ret_t SetCallback(RclEntityT * entity, rcl_event_callback_t callback, const void * user_data) = 0;
    virtual RclEntityT * rcl_ptr() const = 0;
private:
    std::shared_ptr<py::object> callback_;
};


} // namespace rclpy
