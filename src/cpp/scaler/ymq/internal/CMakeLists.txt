target_sources(ymq_objs PRIVATE
    websocket_utils.h
    websocket_utils.cpp

    client.h
    client.cpp

    accept_server.h
    accept_server.cpp

    connect_client.h
    connect_client.cpp

    event_loop_thread.h
    event_loop_thread.cpp

    message_connection.h
    message_connection.cpp

    websocket_stream.h
    websocket_stream.cpp
)

if(WIN32)
    target_sources(ymq_objs PRIVATE event_loop_thread_windows.cpp)
else()
    target_sources(ymq_objs PRIVATE event_loop_thread_unix.cpp)
endif()
