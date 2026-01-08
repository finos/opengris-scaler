# Try to find libuv via CMake config first (works on Windows when libuv is built via CMake)
find_package(libuv CONFIG QUIET)

if(libuv_FOUND)
    # libuv found via CMake config
    set(LIBUV_TARGET libuv::uv)
else()
    # Fall back to pkg-config (works on Linux/macOS)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(libuv REQUIRED IMPORTED_TARGET libuv)
    set(LIBUV_TARGET PkgConfig::libuv)
endif()

add_library(scaler_uv OBJECT
    async.h
    async.cpp

    error.h
    error.cpp

    handle.h

    loop.h
    loop.cpp

    # pipe.h
    # pipe.cpp

    signal.h
    signal.cpp

    # stream.h

    # tcp.h
    # tcp.cpp

    timer.h
    timer.cpp
)

target_link_libraries(scaler_uv PUBLIC ${LIBUV_TARGET})
