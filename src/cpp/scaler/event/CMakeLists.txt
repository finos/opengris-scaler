# FIXME: PkgConfig might not work on non-unixes
find_package(PkgConfig REQUIRED)
pkg_check_modules(UV REQUIRED libuv)

add_library(scaler_event OBJECT
    async.h
    async.cpp

    error.h
    error.cpp

    handle.h

    loop.h
    loop.cpp

    pipe.h

    signal.h

    stream.h

    tcp.h

    timer.h
)
