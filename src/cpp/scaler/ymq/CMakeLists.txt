add_library(ymq_objs OBJECT
    address.h
    address.cpp

    bytes.h

    common.h

    binder_socket.h
    binder_socket.cpp

    connector_socket.h
    connector_socket.cpp

    io_context.h
    io_context.cpp

    message.h

    timestamp.h
)

target_link_libraries(ymq_objs PUBLIC scaler_wrapper_uv)

add_subdirectory(internal)
add_subdirectory(future)
add_subdirectory(sync)

add_subdirectory(pymod)
