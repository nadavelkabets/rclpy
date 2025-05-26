#ifndef RCLPY__OBSERVABLE_HPP_
#define RCLPY__OBSERVABLE_HPP_

#include "pybind11/pybind11.h"
#include "rcl/rcl.h"

namespace py = pybind11;

namespace rclpy {

extern "C" void RclPyCallbackTrampoline(const void * user_data, size_t number_of_events);

struct CallbackContainer {
    std::optional<py::object> cb;
};

class ObservableInterface {
    public:
        virtual void set_callback(py::object) = 0;
        virtual void clear_callback() = 0;
};

void define_abstract_observable(py::object module);

template <class RclEntityT>
class Observable : public ObservableInterface {
public:
    Observable()
    : callback_container_(std::make_shared<CallbackContainer>())
    {
    }

    virtual ~Observable()
    {   
        clear_callback();
    }

    void set_callback(py::object callback) override
    {
        clear_callback();
        callback_container_->cb = std::move(callback);
        SetCallback(rcl_ptr(), RclPyCallbackTrampoline, callback_container_.get());
    }

    void clear_callback() override
    {
        SetCallback(rcl_ptr(), nullptr, nullptr);
        if (callback_container_->cb)
        {
            callback_container_->cb.reset();
        }
    }
    virtual rcl_ret_t SetCallback(RclEntityT * entity, rcl_event_callback_t callback, const void * user_data) = 0;
    virtual RclEntityT * rcl_ptr() const = 0;
private:
    std::shared_ptr<CallbackContainer> callback_container_;
};
} // namespace rclpy

#endif // RCLPY__OBSERVABLE_HPP_
