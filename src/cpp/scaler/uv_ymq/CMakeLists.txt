add_library(uv_ymq_objs OBJECT
    address.h
    address.cpp

    binder_socket.h
    binder_socket.cpp

    connector_socket.h
    connector_socket.cpp

    io_context.h
    io_context.cpp
)

add_subdirectory(internal)
add_subdirectory(future)
add_subdirectory(sync)
