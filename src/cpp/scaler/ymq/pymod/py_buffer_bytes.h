#pragma once

#include <memory>

#include "scaler/utility/pymod/compatibility.h"
#include "scaler/utility/pymod/gil.h"
#include "scaler/ymq/bytes.h"

namespace scaler {
namespace ymq {
namespace pymod {

class PyBufferBytes final: public scaler::ymq::Bytes {
public:
    explicit PyBufferBytes(Py_buffer view): _view(std::make_unique<Py_buffer>(view))
    {
    }

    ~PyBufferBytes() noexcept override
    {
        if (_view) {
            scaler::utility::pymod::AcquireGIL _;
            PyBuffer_Release(_view.get());
        }
    }

    PyBufferBytes(PyBufferBytes&& other) noexcept : _view(std::move(other._view))
    {
    }

    PyBufferBytes& operator=(PyBufferBytes&& other) noexcept
    {
        _view = std::move(other._view);
        return *this;
    }

    const uint8_t* data() const noexcept override
    {
        return static_cast<const uint8_t*>(_view->buf);
    }

    uint8_t* data() noexcept override
    {
        return static_cast<uint8_t*>(_view->buf);
    }

    size_t size() const noexcept override
    {
        return static_cast<size_t>(_view->len);
    }

private:
    std::unique_ptr<Py_buffer> _view;
};

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler
