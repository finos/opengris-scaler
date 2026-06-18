#pragma once

#include <memory>
#include <optional>
#include <string>

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

    PyBufferBytes(PyBufferBytes&&) noexcept            = default;
    PyBufferBytes& operator=(PyBufferBytes&&) noexcept = default;

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

    std::optional<std::string> asString() const override
    {
        if (!data())
            return std::nullopt;
        return std::string(reinterpret_cast<const char*>(data()), size());
    }

private:
    std::unique_ptr<Py_buffer> _view;
};

}  // namespace pymod
}  // namespace ymq
}  // namespace scaler
