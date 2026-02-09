add_library(uv_ymq_objs OBJECT
    accept_server.h
    accept_server.cpp

    address.h
    address.cpp

    binder_socket.h
    binder_socket.cpp

    connect_client.h
    connect_client.cpp

    event_loop_thread.h
    event_loop_thread.cpp

    io_context.h
    io_context.cpp

    message_connection.h
    message_connection.cpp
)
