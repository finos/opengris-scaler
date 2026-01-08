find_package(PkgConfig REQUIRED)
pkg_check_modules(libuv REQUIRED IMPORTED_TARGET libuv)

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

target_link_libraries(scaler_uv PUBLIC PkgConfig::libuv)
