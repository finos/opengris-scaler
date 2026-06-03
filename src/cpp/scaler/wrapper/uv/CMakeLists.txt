find_package(libuv QUIET)
if(libuv_FOUND)
    # Prefer the static archive on every platform so dependents (.pyd, executables) have no
    # runtime libuv DLL/.so dependency. The Win32 system libraries libuv needs (psapi, ws2_32,
    # ...) are propagated via INTERFACE_LINK_LIBRARIES on the imported target.
    set(LIBUV_TARGET libuv::uv_a)
else()
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(libuv REQUIRED IMPORTED_TARGET libuv)
    set(LIBUV_TARGET PkgConfig::libuv)
endif()

add_library(scaler_wrapper_uv STATIC
    async.h
    async.cpp

    error.h
    error.cpp

    handle.h

    loop.h
    loop.cpp

    pipe.h
    pipe.cpp

    signal.h
    signal.cpp

    socket_address.h
    socket_address.cpp

    stream.h

    tcp.h
    tcp.cpp

    timer.h
    timer.cpp
)

target_link_libraries(scaler_wrapper_uv PUBLIC ${LIBUV_TARGET})
